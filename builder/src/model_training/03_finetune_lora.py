"""
Kaggle Notebooks (2x T4 GPU) uchun QLoRA Fine-Tuning Skripti
============================================================
trl kutubxonasiz - faqat transformers.Trainer ishlatiladi (eng barqaror usul)
"""

import os, json

# =================================================================
# 1. Kutubxonalarni o'rnatish
# =================================================================
print("Kutubxonalar o'rnatilmoqda...")
os.system('pip install -q -U transformers datasets peft accelerate bitsandbytes')
print("O'rnatildi!")

import torch
from datasets import load_dataset
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    BitsAndBytesConfig,
    TrainingArguments,
    Trainer,
    DataCollatorForLanguageModeling,
)
from peft import LoraConfig, get_peft_model, prepare_model_for_kbit_training

# =================================================================
# 2. Sozlamalar
# =================================================================
MODEL_NAME = "Qwen/Qwen2.5-7B-Instruct"
DATASET_PATH = "/kaggle/input/datasets/bunyodbek7/dataset1/train.jsonl"
OUTPUT_DIR = "./operator-lora-model"
MAX_LENGTH = 1024

print(f"Model: {MODEL_NAME}")
print(f"GPU: {torch.cuda.device_count()} ta")

# =================================================================
# 3. Tokenizer
# =================================================================
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, trust_remote_code=True)
if tokenizer.pad_token is None:
    tokenizer.pad_token = tokenizer.eos_token
tokenizer.padding_side = "right"

# =================================================================
# 4. Model yuklash (4-bit)
# =================================================================
bnb_config = BitsAndBytesConfig(
    load_in_4bit=True,
    bnb_4bit_use_double_quant=True,
    bnb_4bit_quant_type="nf4",
    bnb_4bit_compute_dtype=torch.float16
)

print("Model yuklanmoqda...")
model = AutoModelForCausalLM.from_pretrained(
    MODEL_NAME,
    quantization_config=bnb_config,
    device_map="auto",
    max_memory={0: "10GB", 1: "10GB"},
    trust_remote_code=True
)

model.config.use_cache = False
model = prepare_model_for_kbit_training(model, use_gradient_checkpointing=True)

# =================================================================
# 5. LoRA qo'llash
# =================================================================
peft_config = LoraConfig(
    r=16,
    lora_alpha=32,
    target_modules=["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"],
    lora_dropout=0.05,
    bias="none",
    task_type="CAUSAL_LM"
)

model = get_peft_model(model, peft_config)
model.print_trainable_parameters()

# =================================================================
# 6. Ma'lumotlarni tayyorlash va tokenizatsiya
# =================================================================
print("Ma'lumotlar yuklanmoqda...")
dataset = load_dataset("json", data_files=DATASET_PATH, split="train")

def tokenize_function(example):
    messages = example["messages"]
    for msg in messages:
        if not isinstance(msg["content"], str):
            msg["content"] = json.dumps(msg["content"], ensure_ascii=False)
    text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=False)
    tokenized = tokenizer(text, truncation=True, max_length=MAX_LENGTH, padding="max_length")
    tokenized["labels"] = tokenized["input_ids"].copy()
    return tokenized

dataset = dataset.map(tokenize_function, remove_columns=dataset.column_names)
print(f"Jami {len(dataset)} ta dialog tayyor!")

# =================================================================
# 7. O'qitish
# =================================================================
print("O'qitish boshlandi...")

training_args = TrainingArguments(
    output_dir=OUTPUT_DIR,
    per_device_train_batch_size=2,
    gradient_accumulation_steps=4,
    optim="paged_adamw_32bit",
    save_steps=100,
    logging_steps=10,
    learning_rate=2e-4,
    weight_decay=0.001,
    fp16=True,
    max_grad_norm=0.3,
    max_steps=500,
    warmup_ratio=0.03,
    lr_scheduler_type="constant",
    report_to="none",
)

trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=dataset,
    data_collator=DataCollatorForLanguageModeling(tokenizer=tokenizer, mlm=False),
)

trainer.train()

# =================================================================
# 8. Saqlash
# =================================================================
print("Model saqlanmoqda...")
trainer.model.save_pretrained(OUTPUT_DIR)
tokenizer.save_pretrained(OUTPUT_DIR)
print("Tayyor! Kaggle'dan yuklab olishingiz mumkin.")
