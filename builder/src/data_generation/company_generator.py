import os
import sys
import json
import random
import time
from dotenv import load_dotenv
from groq import Groq

base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
dotenv_path = os.path.join(base_dir, ".env")
load_dotenv(dotenv_path)

client = Groq(api_key=os.environ.get("GROQ_API_KEY"))

LANGUAGES = ["uz", "ru", "en"]

def build_system_prompt(company_data):
    """Kompaniya ma'lumotlaridan tizim (system) promptini yaratish"""
    prompt = f"Sen '{company_data['name']}' korxonasining sun'iy intellekt operatorisan.\n"
    prompt += f"Soha: {company_data['domain']}.\n"
    prompt += f"Manzil: {company_data['address']}.\n"
    prompt += f"Telefon: {company_data['phone']}.\n"
    prompt += f"Ish vaqti: {company_data['working_hours']}.\n\n"
    
    prompt += "Bizning bo'limlarimiz:\n"
    for dep in company_data.get('departments', []):
         prompt += f"- {dep}\n"
         
    prompt += "\nTez-tez beriladigan savollar (FAQ):\n"
    for faq in company_data.get('faq', []):
         prompt += f"- Savol: {faq['q']}\n  Javob: {faq['a']}\n"
         
    prompt += "\nQoidalar:\n1. Faqat shu korxona haqida javob ber.\n2. FAQ'da bo'lmagan narsani o'zingdan to'qib chiqarma, bilmayman deb ayt.\n3. Mijoz qisqa va aniq javob kutmoqda."
    return prompt

def generate_company_dialog(lang, company_data, system_prompt):
    """Korxona haqida mijoz savoli va operator javobini generatsiya qilish"""
    q_types = ["general_info", "department_routing", "faq_question", "out_of_domain"]
    q_type = random.choice(q_types)
    
    prompt = f"""You are generating a training dataset for an AI receptionist.
The company is: {company_data['name']} (Domain: {company_data['domain']}).
The language must be strictly: {lang}.

Generate ONE realistic dialog turn (user asks, assistant answers).
The dialog type should focus on: {q_type}.
If out_of_domain, the user asks about weather, history, or another company, and the assistant politely refuses based on its role.
If faq_question, use one of the facts from the company data.

Format EXACTLY as a JSON array (no markdown):
[
  {{"role": "user", "content": "..."}},
  {{"role": "assistant", "content": "..."}}
]"""

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=1024
            )
            content = response.choices[0].message.content.strip()
            if content.startswith("```json"):
                content = content[7:-3]
            elif content.startswith("```"):
                content = content[3:-3]
            
            dialog_turns = json.loads(content.strip())
            time.sleep(2) # rate limit
            return dialog_turns
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(5)
    return None

def main(company_json_path, num_samples_per_lang=50):
    with open(company_json_path, "r", encoding="utf-8") as f:
        company_data = json.load(f)
        
    company_id = company_data["company_id"]
    system_prompt = build_system_prompt(company_data)
    
    output_dir = os.path.join(base_dir, "data", "companies_dataset")
    os.makedirs(output_dir, exist_ok=True)
    
    for lang in LANGUAGES:
        output_file = os.path.join(output_dir, f"{company_id}_{lang}.jsonl")
        print(f"[{company_id}] {lang.upper()} tili uchun {num_samples_per_lang} ta dialog yaratilmoqda...")
        
        with open(output_file, "w", encoding="utf-8") as f:
            count = 0
            for _ in range(num_samples_per_lang):
                turns = generate_company_dialog(lang, company_data, system_prompt)
                if turns:
                    full_dialog = {
                        "messages": [{"role": "system", "content": system_prompt}] + turns
                    }
                    f.write(json.dumps(full_dialog, ensure_ascii=False) + "\n")
                    f.flush()
                    count += 1
            print(f"[{lang}] {count} ta dialog saqlandi: {output_file}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        c_path = sys.argv[1]
    else:
        c_path = os.path.join(base_dir, "data", "companies", "medline.json")
    
    if os.path.exists(c_path):
        # Test uchun tezroq bo'lishi uchun 10 ta qilamiz
        main(c_path, num_samples_per_lang=10)
    else:
        print(f"Topilmadi: {c_path}")
