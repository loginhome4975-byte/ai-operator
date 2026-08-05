import os
import requests
import sys

ORCHESTRATOR_URL = "http://127.0.0.1:8080"
API_KEY = os.environ.get("ORCHESTRATOR_API_KEY")
if not API_KEY:
    print("FATAL: ORCHESTRATOR_API_KEY environment o'zgaruvchisi talab qilinadi.", file=sys.stderr, flush=True)
    sys.exit(2)  # _abort_startup bilan bir xil exit code (orchestrator bilan consistency)

print("==================================================")
print("  Kaggle LLM bilan to'g'ridan-to'g'ri muloqot!  ")
print("  Chiqish uchun 'quit', 'exit' yoki 'q' yozing  ")
print("==================================================")

while True:
    try:
        user_input = input("\nSiz: ")
        if user_input.lower() in ['quit', 'exit', 'q']:
            print("Chat yakunlandi.")
            break
            
        if not user_input.strip():
            continue

        print("LLM o'ylanmoqda... (Kaggle orqali)")
        
        response = requests.post(
            f"{ORCHESTRATOR_URL}/api/chat_text",
            headers={
                "X-API-Key": API_KEY,
                "Content-Type": "application/json"
            },
            json={
                "caller_id": "cli_user",
                "language": "uz",
                "text": user_input
            },
            timeout=120
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"\nKaggle LLM: {data.get('ai_response', 'JAVOB YOQ')}")
        else:
            print(f"\nXatolik yuz berdi: {response.status_code} - {response.text}")
            
    except KeyboardInterrupt:
        print("\nChat yakunlandi.")
        break
    except Exception as e:
        print(f"\nUlanishda xatolik: {e}")
