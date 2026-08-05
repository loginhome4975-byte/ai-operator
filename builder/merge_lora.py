import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
from peft import PeftModel
import os
import shutil

base_model_id = "Qwen/Qwen2.5-7B-Instruct"
lora_dir = "/home/ubuntu/ai-operator/builder/models/lora_output"
merged_dir = "/home/ubuntu/ai-operator/builder/models/merged_model"

print(f"[1/4] Asosiy model ({base_model_id}) yuklanmoqda...")
# Qwen 7B model
base_model = AutoModelForCausalLM.from_pretrained(
    base_model_id,
    torch_dtype=torch.float16,
    device_map="cpu", # CPU serverdamiz
    low_cpu_mem_usage=True
)
tokenizer = AutoTokenizer.from_pretrained(base_model_id)

print(f"[2/4] LoRA adapterlar ulanmoqda ({lora_dir})...")
model = PeftModel.from_pretrained(base_model, lora_dir)

print("[3/4] Modellar birlashtirilmoqda (Merging)...")
merged_model = model.merge_and_unload()

print(f"[4/4] Birlashtirilgan model saqlanmoqda: {merged_dir}")
os.makedirs(merged_dir, exist_ok=True)
merged_model.save_pretrained(merged_dir, safe_serialization=True)
tokenizer.save_pretrained(merged_dir)

# chat_template.jinja ni ham nusxalab qoyish
chat_template_path = os.path.join(lora_dir, "chat_template.jinja")
if os.path.exists(chat_template_path):
    shutil.copy(chat_template_path, os.path.join(merged_dir, "chat_template.jinja"))

print("\nBirlashtirish (Merge) MUVAFAQQIYATLI YAKUNLANDI! ✅")
