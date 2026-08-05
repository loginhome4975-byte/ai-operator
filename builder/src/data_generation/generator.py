import os
import json
import random
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from openai import OpenAI

# .env faylini yuklash
base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
dotenv_path = os.path.join(base_dir, ".env")
load_dotenv(dotenv_path)

# API kalitlarni olish
api_key = os.environ.get("CEREBRAS_API_KEY", "").strip()

if not api_key:
    print("XATO: CEREBRAS_API_KEY topilmadi! Iltimos .env ga qo'shing.")
    exit(1)

# Cerebras OpenAI klienti
client = OpenAI(
    base_url="https://api.cerebras.ai/v1",
    api_key=api_key
)

LANGUAGES = ["uzbek", "russian", "english"]
COMPANIES = ["MedLine Clinic", "TexnoServis", "AutoTrade LLC", "EduCenter", "FastDelivery"]

SKILLS = [
    # A. Suhbat boshlash va yakunlash
    "Skill 1: Salomlashish (Professional, iliq salomlashish; vaqtga mos)",
    "Skill 2: Xayrlashish (Suhbatni to'g'ri yakunlash, keyingi qadamni aytish)",
    "Skill 3: Kutish va qayta aloqa (Mijozni kutishga qo'yish, bir daqiqa kuting)",
    "Skill 4: Suhbat xulosasi (Suhbat oxirida qisqacha xulosa va keyingi qadamni aytish)",
    # B. Til va muloqot
    "Skill 5: Til tanlash (O'zbek, Rus yoki Ingliz tilini tanlash bo'yicha yordam)",
    "Skill 6: Tilni qayta tanlash (Boshqa tilga o'tishni so'rash)",
    "Skill 7: Noaniq gapni aniqlashtirish (Tushunarsiz yoki to'liq bo'lmagan gapni qayta so'rash)",
    # C. Ma'lumot berish
    "Skill 8: FAQ javob berish (Tez-tez beriladigan umumiy savollarga javob berish)",
    "Skill 9: Korxona haqida ma'lumot (Manzil, aloqa, xizmatlar ro'yxati)",
    "Skill 10: Narx va xizmat turlari (Xizmat narxlari va turlari haqida ma'lumot berish)",
    # D. Yo'naltirish
    "Skill 11: Bo'limga yo'naltirish (route_to_department tool call orqali tegishli bo'limga yo'naltirish)",
    "Skill 12: Xodimga yo'naltirish (Aniq xodimga yoki ichki raqamga bog'lash)",
    "Skill 13: Odam operatorga uzatish (Model hal qila olmasa - escalate_to_human tool call)",
    # E. Qabul va yozish
    "Skill 14: Ma'lumot to'plash (Ism, telefon, email kabi ma'lumotlarni so'rash)",
    "Skill 15: Ma'lumotni tasdiqlash (Yig'ilgan ma'lumotlarni takrorlab tasdiqlatish)",
    "Skill 16: Ariza qabul qilish (create_request tool call orqali arizani ro'yxatdan o'tkazish)",
    "Skill 17: Buyurtma qabul qilish (create_order tool call orqali buyurtma olish)",
    "Skill 18: Shikoyat qabul qilish (Shikoyatni sabr bilan qabul qilish va yozib olish)",
    # F. Vaqt va navbat
    "Skill 19: Navbat belgilash (schedule_appointment tool call orqali navbatga yozish)",
    "Skill 20: Qayta qo'ng'iroq vaqti (Qachon qayta qo'ng'iroq qilishni kelishib olish)",
    # G. Maxsus holatlar
    "Skill 21: Favqulodda holat (Shoshilinch holatlarni tan olish va tez harakat qilish)",
    "Skill 22: Xatolik va uzr (O'z xatosini tan olish, uzr so'rash, tuzatish)",
    "Skill 23: Maxfiylik va ruxsat (Shaxsiy ma'lumot yig'ishdan oldin ruxsat so'rash)",
    # Qoshimcha Qoida
    "Skill Tabula Rasa: Ob-havo, tarix kabi korxonaga xos bo'lmagan narsalar so'ralsa rad etish"
]

SYSTEM_PROMPT_TEMPLATE = """Sen {company} korxonasining aqlli ofis operatorisan. Qisqa va aniq javob ber. 
Seni vazifang xushmuomala bo'lish va faqat korxona xizmatlari doirasida yordam berish.
Agar foydalanuvchi boshqa ma'lumot so'rasa, bilmasligingni ayt."""

write_lock = threading.Lock()

def generate_single_dialog(task_id):
    """Bitta dialogni generatsiya qilish funksiyasi (Thread ichida ishlaydi)"""
    lang = random.choice(LANGUAGES)
    company = random.choice(COMPANIES)
    skill = random.choice(SKILLS)
    
    prompt = f"""You are an expert conversational AI dataset generator.
Create a single realistic phone call dialog snippet between a User and a Receptionist for a company named '{company}'.
Language of the dialog MUST be: {lang}.
The dialog MUST demonstrate the following skill: {skill}

Requirements:
- The Receptionist must be polite, concise, and helpful.
- If the skill involves a tool call (like create_request or route_to_department), the Receptionist's final turn MUST be exactly the JSON string for the tool call and NOTHING else.
- Output ONLY the raw JSON array format for the dialog. No markdown blocks, no explanations.

Expected strictly valid JSON schema format:
[
  {{"role": "user", "content": "user's message"}},
  {{"role": "assistant", "content": "assistant's message or JSON tool call"}}
]
"""
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Cerebras API juda tez va gpt-oss-120b modelini ishlatadi
            response = client.chat.completions.create(
                model="gpt-oss-120b",
                messages=[
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=2048,
            )
            
            content = response.choices[0].message.content.strip()
            
            # Markdown belgilarini tozalash
            if content.startswith("```json"):
                content = content[7:-3].strip()
            elif content.startswith("```"):
                content = content[3:-3].strip()
                
            dialog_turns = json.loads(content)
            
            # Muvaffaqiyatli bo'lsa natijani qaytaramiz
            system_msg = {"role": "system", "content": SYSTEM_PROMPT_TEMPLATE.format(company=company)}
            full_dialog = {"messages": [system_msg] + dialog_turns}
            
            # Cerebras API juda tez bo'lgani bilan, uning daqiqalik (RPM) limiti bor.
            # Limittan oshib ketmaslik uchun 4 soniya kutamiz
            time.sleep(4)
            return full_dialog
            
        except Exception as e:
            err_msg = str(e)
            print(f"Cerebras API Xatosi: {err_msg}")
            if "429" in err_msg or "rate" in err_msg.lower() or "quota" in err_msg.lower():
                time.sleep(10)
            else:
                time.sleep(2)
                
    return None

def main(target_total=1000):
    data_dir = os.path.join(base_dir, "data", "operator_dataset")
    os.makedirs(data_dir, exist_ok=True)
    output_file = os.path.join(data_dir, "train.jsonl")
    
    existing_count = 0
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8') as f:
            existing_count = sum(1 for _ in f)
            
    num_samples = target_total - existing_count
    
    if num_samples <= 0:
        print(f"Bazada allaqachon {existing_count} ta dialog bor. Topshiriq yakunlangan.")
        return
        
    print(f"Cerebras API ishga tushdi.")
    print(f"Bazada {existing_count} ta mavjud. Yana {num_samples} ta dialog generatsiya qilinmoqda...")
    
    # Cerebras default RPM limitini (odatda 30-60 RPM) hisobga olib workerlarni kamaytiramiz
    max_workers = 2
    
    success_count = 0
    
    with open(output_file, "a", encoding="utf-8") as f:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(generate_single_dialog, i) for i in range(num_samples)]
            
            for future in as_completed(futures):
                result = future.result()
                if result:
                    with write_lock:
                        f.write(json.dumps(result, ensure_ascii=False) + "\n")
                        f.flush()
                        success_count += 1
                        if success_count % 10 == 0:
                            print(f"[{existing_count + success_count}/{target_total}] muvaffaqiyatli yozildi...")

    print(f"\nTugallandi! Topshiriq bo'yicha {success_count} ta yangi dialog {output_file} ga saqlandi.")

if __name__ == "__main__":
    main(1000)
