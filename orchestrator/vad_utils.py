import os
import torch
import warnings
import numpy as np

warnings.filterwarnings("ignore")

# Audit xulosasi: Silero chunk-level tekshirishda shu muhitda deterministik
# emas va 512/256-sample oynalar, int16/float input, threshold sezgirligi tufayli
# pipeline'ni o'lik qilib qo'ygan. Endi DEFAULT: energy-based (RMS) — deterministik,
# tez va telefon audio (8kHz/16kHz) uchun yetarli. Silero faqat SILERO_VAD=1
# env bilan ulangan bo'lsa ishlatiladi (ixtiyoriy kuchaytirish).

_SILERO_ENABLED = os.getenv("SILERO_VAD", "0").lower() in ("1", "true", "yes")


class SileroVAD:
    """Voice Activity Detection.

    is_speech(audio_bytes, sampling_rate):
      - Sukut RMS < 0.02 (~-34 dBFS) → False
      - Ovoz RMS >= 0.02 → True
    Silero real modeli faqat SILERO_VAD=1 da yuklanadi va ishlatiladi.
    """
    def __init__(self, sampling_rate=16000):
        self.sampling_rate = sampling_rate
        self._acc = []
        self._last_speech = False
        self.use_real_model = False

        if _SILERO_ENABLED:
            try:
                self.model, utils = torch.hub.load(
                    repo_or_dir='snakers4/silero-vad', model='silero_vad',
                    force_reload=False, trust_repo=True)
                (self.get_speech_timestamps, self.save_audio, self.read_audio,
                 self.VADIterator, self.collect_chunks) = utils
                self.use_real_model = True
            except Exception as e:
                print(f"[VAD Warning] Silero yuklanmadi, energy rejim ishlatiladi: {e}")
                self.use_real_model = False

    def is_speech(self, audio_bytes: bytes, sampling_rate: int = 16000) -> bool:
        """Audio bo'lakda inson ovozi bor-yo'qligini aniqlaydi."""
        if not audio_bytes:
            return False
        try:
            audio_i16 = np.frombuffer(audio_bytes, dtype=np.int16)
        except Exception:
            return True
        if len(audio_i16) == 0:
            return self._last_speech

        # ── ENERGY (default, deterministik) ──
        audio_np = audio_i16.astype(np.float32) / 32768.0
        rms = float(np.sqrt(np.mean(audio_np ** 2)))
        if not self.use_real_model:
            return rms > 0.02

        # ── SILERO (ixtiyoriy, SILERO_VAD=1) ──
        # Model 1D int16 tensor kutadi; 16k→512 sample, 8k→256 sample oynalar.
        window = 256 if sampling_rate == 8000 else 512
        self._acc.extend(audio_i16.tolist())
        while len(self._acc) >= window:
            chunk = np.array(self._acc[:window], dtype=np.int16)
            self._acc = self._acc[window:]
            try:
                prob = self.model(torch.from_numpy(chunk), sampling_rate).item()
                self._last_speech = prob > 0.35
            except Exception:
                # Model xato bersa — energy natija orqali baholash
                self._last_speech = rms > 0.02
        return self._last_speech


vad_model = SileroVAD()
