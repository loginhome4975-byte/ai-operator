import io
import logging
import wave as _wave
from typing import Tuple

log = logging.getLogger("audio_utils")


def _parse_wav_header(audio_bytes: bytes) -> Tuple[int, int, int, int]:
    """WAV header'dan metadata o'qishni xavfsiz qiladi.
    Qaytaradi: (n_channels, sample_width, frame_rate, n_frames).
    Xato bo'lsa except'da log qiladi va maxsus qiymat qaytaradi."""
    try:
        with io.BytesIO(audio_bytes) as audio_io:
            with _wave.open(audio_io, "rb") as wav_file:
                return (
                    wav_file.getnchannels(),
                    wav_file.getsampwidth(),
                    wav_file.getframerate(),
                    wav_file.getnframes(),
                )
    except Exception as e:
        log.debug(f"WAV header parse xatosi: {e}")
        return (0, 0, 0, 0)


def ensure_wav_16k_mono(audio_bytes: bytes) -> bytes:
    """Kelayotgan audio baytlarni STT modullari (16kHz, Mono WAV) uchun mos holatga keltiradi.

    - Tayyor 16kHz mono 16-bit WAV bo'lsa — o'zgartirmasdan qaytaradi.
    - Boshqa formatlar bo'lsa — pydub yordamida konvertatsiya qiladi.
    - Pydub yo'q bo'lsa — asl bytes qaytaradi (STT keyin xato ko'rsatadi).
    """
    if not audio_bytes:
        return b""

    ch, sw, fr, _n = _parse_wav_header(audio_bytes)
    if ch == 1 and sw == 2 and fr == 16000:
        return audio_bytes

    # Pydub yoki FFmpeg yordamida konvertatsiya
    try:
        from pydub import AudioSegment
        segment = AudioSegment.from_file(io.BytesIO(audio_bytes))
        segment = segment.set_frame_rate(16000).set_channels(1).set_sample_width(2)
        out_buf = io.BytesIO()
        segment.export(out_buf, format="wav")
        return out_buf.getvalue()
    except Exception as e:
        log.warning(f"Pydub/ffmpeg konversiyasi ishlamadi, asl audio qaytarildi: {e}")
        return audio_bytes


def wav_to_pcm(audio_bytes: bytes, target_rate: int = 16000) -> bytes:
    """WAV baytlaridan raw PCM (mono, 16-bit, target_rate) ajratib oladi.

    - Har qanday WAV (sample_width 1/2/4, stereo/mono, istalgan rate) qabul qilinadi.
    - Sample rate o'zgartiriladi (numpy interp, linear resample).
    - Konversiya imkoni bo'lmasa yoki audio buzilgan bo'lsa b'' qaytaradi.
    - SIP path (RTP ulaw, 8kHz) va WS path (16kHz) uchun bitta manba.
    """
    if not audio_bytes:
        return b""
    try:
        with io.BytesIO(audio_bytes) as buf:
            with _wave.open(buf, "rb") as w:
                sampwidth = w.getsampwidth()
                channels = w.getnchannels()
                framerate = w.getframerate()
                raw = w.readframes(w.getnframes())
    except Exception as e:
        log.debug(f"wav_to_pcm: WAV ochilmadi: {e}")
        return b""

    # Sample width → 16-bit
    if sampwidth != 2:
        try:
            import audioop
            if sampwidth == 1:
                # audioop.bias(fragment, width, bias) — 3 argument kerak
                raw = audioop.bias(raw, 1, 128)
            elif sampwidth == 4:
                raw = audioop.lin2lin(raw, 4, 2)
            else:
                return b""
            sampwidth = 2
        except Exception as e:
            log.debug(f"wav_to_pcm: sampwidth convert skip: {e}")
            return b""

    # Stereo → mono
    if channels > 1:
        try:
            import audioop
            raw = audioop.tomono(raw, sampwidth, 1, 0)
            channels = 1
        except Exception as e:
            log.debug(f"wav_to_pcm: tomono skip: {e}")
            return b""

    # Rate → target_rate
    if framerate != target_rate:
        try:
            import numpy as np
            samples = np.frombuffer(raw, dtype=np.int16).astype(np.float32)
            n = len(samples)
            if n == 0:
                return b""
            target_n = int(round(n * target_rate / float(framerate)))
            if target_n <= 0:
                return b""
            xp = np.linspace(0.0, 1.0, num=n)
            x = np.linspace(0.0, 1.0, num=target_n)
            resampled = np.interp(x, xp, samples)
            return resampled.astype(np.int16).tobytes()
        except Exception as e:
            log.debug(f"wav_to_pcm: numpy resample unavailable: {e}")
            return b""

    if sampwidth == 2 and channels == 1:
        return raw
    return b""
