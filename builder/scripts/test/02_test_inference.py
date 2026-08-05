import os
import json
from llama_cpp import Llama

def test_inference():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    model_path = os.path.join(base_dir, "models", "qwen2.5-7b-instruct-q4_k_m-00001-of-00002.gguf")
    
    if not os.path.exists(model_path):
        print(f"Xato: Model topilmadi: {model_path}")
        print("Iltimos, avval 01_download_qwen.py skriptini ishga tushiring.")
        return

    print("Model yuklanmoqda... (bu biroz vaqt olishi mumkin)")
    # CPU parametrlariga moslashtiramiz (16 vCPU)
    llm = Llama(
        model_path=model_path,
        n_ctx=2048,
        n_threads=8, # 16 vCPU uchun 8-12 optimal
        verbose=False
    )
    
    print("\n--- 1. O'zbek tilida test ---")
    prompt_uz = "<|im_start|>system\nSen ofis operatorisan. Qisqa va aniq javob ber.<|im_end|>\n<|im_start|>user\nAssalomu alaykum, bugun qanday ishlayapsizlar?<|im_end|>\n<|im_start|>assistant\n"
    response_uz = llm(prompt_uz, max_tokens=100, stop=["<|im_end|>"])
    print(response_uz["choices"][0]["text"].strip())

    print("\n--- 2. Rus tilida test ---")
    prompt_ru = "<|im_start|>system\nSen ofis operatorisan. Qisqa va aniq javob ber.<|im_end|>\n<|im_start|>user\nЗдравствуйте, как вы работаете сегодня?<|im_end|>\n<|im_start|>assistant\n"
    response_ru = llm(prompt_ru, max_tokens=100, stop=["<|im_end|>"])
    print(response_ru["choices"][0]["text"].strip())

    print("\n--- 3. Ingliz tilida test ---")
    prompt_en = "<|im_start|>system\nSen ofis operatorisan. Qisqa va aniq javob ber.<|im_end|>\n<|im_start|>user\nHello, what are your working hours today?<|im_end|>\n<|im_start|>assistant\n"
    response_en = llm(prompt_en, max_tokens=100, stop=["<|im_end|>"])
    print(response_en["choices"][0]["text"].strip())

    print("\n--- 4. Tool Calling Test (JSON format) ---")
    prompt_tool = """<|im_start|>system
Sen aqlli ofis operatorisan. Agar foydalanuvchi xodim bilan ulanishni so'rasa, quyidagi JSON formatida javob qaytar:
{"tool": "connect_agent", "department": "bolim_nomi"}
Agar boshqa narsa so'rasa, oddiy matn bilan javob ber.
<|im_end|>
<|im_start|>user
Meni savdo bo'limi bilan bog'lay olasizmi?<|im_end|>
<|im_start|>assistant
"""
    response_tool = llm(prompt_tool, max_tokens=100, stop=["<|im_end|>"])
    print(response_tool["choices"][0]["text"].strip())
    
    print("\n[+] Barcha testlar yakunlandi.")

if __name__ == "__main__":
    test_inference()
