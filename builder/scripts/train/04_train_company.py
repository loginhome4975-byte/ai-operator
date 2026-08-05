"""
Ushbu skript ma'lum bir korxona uchun generatsiya qilingan (Uz, Ru, En) datasetlarni olib,
operator-base modelidan 3 xil (tilga xos) yangi modellarni o'qitish jarayonini avtomatlashtiradi.
"""

import os
import sys

def mock_train_company(company_id):
    """
    Simulyatsiya: Aslida bu yerda Unsloth orqali SFTTrainer ishga tushishi kerak.
    Biz hozircha arxitekturani tekshiryapmiz.
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    dataset_dir = os.path.join(base_dir, "data", "companies_dataset")
    models_dir = os.path.join(base_dir, "models", "companies")
    os.makedirs(models_dir, exist_ok=True)
    
    languages = ["uz", "ru", "en"]
    
    print(f"=== {company_id.upper()} kompaniyasi uchun modellar o'qitish boshlandi ===")
    
    for lang in languages:
        dataset_path = os.path.join(dataset_dir, f"{company_id}_{lang}.jsonl")
        model_save_path = os.path.join(models_dir, f"{company_id}-{lang}")
        
        if not os.path.exists(dataset_path):
            print(f"[{lang.upper()}] Xato: Dataset topilmadi ({dataset_path})")
            continue
            
        print(f"\n[{lang.upper()}] Dataset yuklanmoqda: {dataset_path}")
        print(f"[{lang.upper()}] operator-base modeliga LoRA qo'llanilmoqda...")
        print(f"[{lang.upper()}] Epochs: 3, Batch: 2, GPU VRAM sarfi: ~8GB")
        
        # Simulyatsiya qilingan vaqt o'tishi
        print(f"[{lang.upper()}] O'qitish ketyapti... (SIMULATION)")
        
        # Tayyor modelni saqlash (Simulyatsiya qilingan papka yaratish)
        os.makedirs(model_save_path, exist_ok=True)
        with open(os.path.join(model_save_path, "adapter_config.json"), "w") as f:
            f.write('{"peft_type": "LORA", "base_model_name_or_path": "operator-base"}')
            
        print(f"[{lang.upper()}] Muvaffaqiyatli saqlandi: {model_save_path}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        c_id = sys.argv[1]
    else:
        c_id = "medline"
        
    mock_train_company(c_id)
