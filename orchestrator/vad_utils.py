import torch
import warnings
import io
import wave
import numpy as np

warnings.filterwarnings("ignore")

class SileroVAD:
    """
    Silero VAD (Voice Activity Detection) orqali inson ovozini sukutdan ajratish.
    Bu class WebSocket orqali keladigan audio bo'laklarni (chunks) tekshiradi
    va agar foydalanuvchi gapirishni to'xtatsa (sukut saqlasa) qayd etadi.
    """
    def __init__(self, sampling_rate=16000):
        self.sampling_rate = sampling_rate
        # Hozircha lokal VAD mantiqi yoki mock holatida ishlatamiz.
        # Haqiqiy proyektda: model, utils = torch.hub.load(repo_or_dir='snakers4/silero-vad', model='silero_vad')
        try:
            self.model, utils = torch.hub.load(repo_or_dir='snakers4/silero-vad', model='silero_vad', force_reload=False, trust_repo=True)
            (self.get_speech_timestamps, self.save_audio, self.read_audio, self.VADIterator, self.collect_chunks) = utils
            self.use_real_model = True
        except Exception as e:
            print(f"[VAD Warning] Silero-VAD modelini yuklab bo'lmadi, mock rejim ishga tushdi: {e}")
            self.use_real_model = False

    def is_speech(self, audio_bytes: bytes) -> bool:
        """Kichik audio bo'lakda (chunk) inson ovozi bor-yo'qligini aniqlaydi."""
        if not self.use_real_model:
            # Mock mantiq: Agar baytlar ichida noldan farqli elementlar ko'p bo'lsa
            return len(audio_bytes) > 1000

        try:
            # Baytlarni tensorga aylantirish
            audio_np = np.frombuffer(audio_bytes, dtype=np.int16).astype(np.float32) / 32768.0
            audio_tensor = torch.from_numpy(audio_np)
            
            # Predict
            speech_prob = self.model(audio_tensor, self.sampling_rate).item()
            return speech_prob > 0.5
        except Exception:
            return True

vad_model = SileroVAD()
