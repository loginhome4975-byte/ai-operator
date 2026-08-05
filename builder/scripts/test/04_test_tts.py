import os
import sys
import time

base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(base_dir)

from src.tts.normalizer import normalize_text
from src.tts.tts_engine import text_to_speech

def test_tts():
    cache_dir = os.path.join(base_dir, "data", "audio_cache")
    os.makedirs(cache_dir, exist_ok=True)
    
    samples = [
        {
            "lang": "uz",
            "text": "Salom! Men sizning AI ofis operatoringizman. Narximiz: 12500 so'm."
        },
        {
            "lang": "ru",
            "text": "Здравствуйте! Я ваш ИИ-оператор. Как я могу вам помочь?"
        },
        {
            "lang": "en",
            "text": "Hello! I am your AI office receptionist. How can I assist you today?"
        }
    ]
    
    for i, sample in enumerate(samples):
        lang = sample["lang"]
        original_text = sample["text"]
        
        print(f"\n--- Test {i+1} [{lang.upper()}] ---")
        print(f"Original matn: {original_text}")
        
        # Normalizatsiya qilamiz
        clean_text = normalize_text(original_text, lang)
        print(f"Tozalangan matn: {clean_text}")
        
        # Audio nomlari
        out_original = os.path.join(cache_dir, f"test_{lang}_original.mp3")
        out_clean = os.path.join(cache_dir, f"test_{lang}_clean.mp3")
        
        # 1. Original (belgilar bilan) audio yaratamiz
        t1 = time.time()
        text_to_speech(original_text, lang, out_original)
        rtf_original = time.time() - t1
        print(f"Original audio saqlandi: {out_original} ({rtf_original:.2f} soniya)")
        
        # 2. Tozalangan matndan audio yaratamiz
        t2 = time.time()
        text_to_speech(clean_text, lang, out_clean)
        rtf_clean = time.time() - t2
        print(f"Tozalangan audio saqlandi: {out_clean} ({rtf_clean:.2f} soniya)")

if __name__ == "__main__":
    print("TTS Sinovlari boshlanmoqda...")
    test_tts()
    print("\nBarcha testlar muvaffaqiyatli yakunlandi!")
