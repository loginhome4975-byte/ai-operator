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
