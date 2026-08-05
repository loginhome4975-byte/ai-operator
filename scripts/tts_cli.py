import os
import sys
import json
import time
import requests

# Orchestrator papkasini path'ga qo'shamiz (security_utils uchun)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'orchestrator')))
try:
    from security_utils import encrypt_payload
except ImportError:
    print("Xatolik: security_utils topilmadi. Skriptni loyiha papkasidan ishga tushiring.")
    sys.exit(1)

def get_kaggle_url():
    """Kaggle URL'ni topish: birinchi ORCHESTRATOR_URL env, keyin KAGGLE_URL env."""
    # Eng oddiy: ORCHESTRATOR_URL env orqali orchestrator'ga so'rash
    orch_url = os.environ.get("ORCHESTRATOR_URL", "http://127.0.0.1:8080")
    api_key = os.environ.get("ORCHESTRATOR_API_KEY", "")
    try:
        resp = requests.get(f"{orch_url}/api/health", headers={"X-API-Key": api_key}, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            live = data.get("live_check", {})
            for name, status in live.items():
                if "LLM" in name and status == "healthy":
                    # URL'ni env'dan olish
                    kaggle_url = os.environ.get("KAGGLE_URL")
                    if kaggle_url:
                        return kaggle_url
    except Exception:
        pass
    return os.environ.get("KAGGLE_URL")

def main():
    print("==================================================")
    print("      Sayro TTS bilan to'g'ridan-to'g'ri sinov!    ")
    print("==================================================")
    
    kaggle_url = get_kaggle_url()
    if not kaggle_url:
        print("Xatolik: Kaggle URL topilmadi. ai-orchestrator.service ishlayotganini tekshiring.")
        sys.exit(1)
        
    print(f"[*] Kaggle tuguni aniqlandi: {kaggle_url}")
    
    print("Dasturdan chiqish uchun 'quit', 'exit' yoki 'q' yozing.\n")
    
    while True:
        text = input("Matn kiriting (uzbek tilida): ")
        if not text.strip():
            continue
        if text.strip().lower() in ['quit', 'exit', 'q']:
            print("Dastur tugatildi.")
            break
            
        print("Sayro TTS ishlayapti... (Ovoz yaratilmoqda, kuting)")
        
        url = f"{kaggle_url}/synthesize"
        payload = {"text": text, "language": "uz"}
        encrypted_str = encrypt_payload(json.dumps(payload).encode('utf-8'))
        req_data = {"encrypted_payload": encrypted_str}
        
        try:
            start_time = time.time()
            response = requests.post(url, json=req_data, timeout=120)
            
            if response.status_code == 200:
                out_file = f"sayro_tts_{int(time.time())}.wav"
                with open(out_file, "wb") as f:
                    f.write(response.content)
                
                elapsed = round(time.time() - start_time, 2)
                print(f"[+] Ajoyib! Ovoz muvaffaqiyatli saqlandi: {out_file}")
                print(f"    (Vaqt: {elapsed} soniya)\n")
            else:
                print(f"Xatolik yuz berdi: {response.status_code} - {response.text}\n")
        except Exception as e:
            print(f"Ulanishda yoki generatsiyada xatolik: {e}\n")

if __name__ == "__main__":
    main()
