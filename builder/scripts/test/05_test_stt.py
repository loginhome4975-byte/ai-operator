import os
import sys
import time

base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(base_dir)

from src.stt.stt_engine import get_stt_engine

def test_stt():
    # 4-bosqichda yaratilgan audiolarni olamiz
    audio_cache_dir = os.path.join(base_dir, "data", "audio_cache")
    
    test_files = [
        {"lang": "uz", "file": "test_uz_clean.mp3"},
        {"lang": "ru", "file": "test_ru_clean.mp3"},
        {"lang": "en", "file": "test_en_clean.mp3"}
    ]
    
    # Engine'ni yuklash (Birinchi marta sal vaqt oladi - model yuklanadi)
    print("STT Engine ishga tushirilmoqda (birinchi yuklanish biroz vaqt oladi)...")
    t0 = time.time()
    engine = get_stt_engine()
    print(f"Engine tayyor bo'lishi uchun ketgan vaqt: {time.time() - t0:.2f} soniya\n")
    
    for item in test_files:
        lang = item["lang"]
        audio_path = os.path.join(audio_cache_dir, item["file"])
        
        if not os.path.exists(audio_path):
            print(f"[{lang.upper()}] Xato: Audio fayl topilmadi - {audio_path}")
            continue
            
        print(f"=== {lang.upper()} tilida test qilinmoqda ===")
        
        # Ovozni matnga o'girish vaqtini o'lchash
        t_start = time.time()
        result_text = engine.transcribe(audio_path, language=lang)
        t_end = time.time()
        
        rtf_time = t_end - t_start
        print(f"Natija: {result_text}")
        print(f"Tezlik (Processing Time): {rtf_time:.2f} soniya\n")

if __name__ == "__main__":
    test_stt()
    print("Barcha STT testlari muvaffaqiyatli yakunlandi!")
