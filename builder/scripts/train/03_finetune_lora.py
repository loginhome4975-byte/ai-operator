"""
Ushbu skript Qwen2.5 modelini operatorlik bo'yicha fine-tuning (LoRA) qilish uchun mo'ljallangan.
Skriptni Cloud GPU serverda (kamida 24GB VRAM) ishga tushirish tavsiya etiladi.
Kutubxonalar: unsloth, trl, peft, transformers, datasets

O'rnatish:
pip install "unsloth[colab-new] @ git+https://github.com/unslothai/unsloth.git"
pip install --no-deps xformers "trl<0.9.0" peft accelerate bitsandbytes
"""

import os
from unsloth import FastLanguageModel
from datasets import load_dataset
from trl import SFTTrainer
from transformers import TrainingArguments

# Parametrlar
max_seq_length = 2048 # Operator javoblari qisqa bo'lgani uchun 2048 yetarli
dtype = None
load_in_4bit = True

# 1. Modelni yuklash
print("Model va tokenizer yuklanmoqda...")
model, tokenizer = FastLanguageModel.from_pretrained(
    model_name = "Qwen/Qwen2.5-7B-Instruct",
    max_seq_length = max_seq_length,
    dtype = dtype,
    load_in_4bit = load_in_4bit,
)

# 2. LoRA sozlamalari
model = FastLanguageModel.get_peft_model(
    model,
    r = 16, # Rank
    target_modules = ["q_proj", "k_proj", "v_proj", "o_proj",
                      "gate_proj", "up_proj", "down_proj",],
    lora_alpha = 16,
    lora_dropout = 0, 
    bias = "none",
    use_gradient_checkpointing = "unsloth",
    random_state = 3407,
    use_rslora = False,
    loftq_config = None,
)

# 3. Datasetni tayyorlash
base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
dataset_path = os.path.join(base_dir, "data", "operator_dataset", "train.jsonl")

# Qwen uchun ShareGPT/ChatML formatlarga o'girish funksiyasi
from unsloth.chat_templates import get_chat_template
tokenizer = get_chat_template(
    tokenizer,
    chat_template = "chatml", 
    mapping = {"role": "role", "content": "content", "user": "user", "assistant": "assistant"}
)

def formatting_prompts_func(examples):
    convos = examples["messages"]
    texts = [tokenizer.apply_chat_template(convo, tokenize = False, add_generation_prompt = False) for convo in convos]
    return { "text" : texts, }

print(f"Dataset yuklanmoqda: {dataset_path}")
dataset = load_dataset("json", data_files=dataset_path, split="train")
dataset = dataset.map(formatting_prompts_func, batched = True,)

# 4. O'qitish (Training)
trainer = SFTTrainer(
    model = model,
    tokenizer = tokenizer,
    train_dataset = dataset,
    dataset_text_field = "text",
    max_seq_length = max_seq_length,
    dataset_num_proc = 2,
    packing = False, # Tezlashtirish uchun True qilsa ham bo'ladi
    args = TrainingArguments(
        per_device_train_batch_size = 2,
        gradient_accumulation_steps = 4,
        warmup_steps = 5,
        max_steps = 60, # Demo uchun 60, real o'qitishda epoch 1-3
        learning_rate = 2e-4,
        fp16 = not getattr(model, "is_bf16_supported", lambda: False)(),
        bf16 = getattr(model, "is_bf16_supported", lambda: False)(),
        logging_steps = 1,
        optim = "adamw_8bit",
        weight_decay = 0.01,
        lr_scheduler_type = "linear",
        seed = 3407,
        output_dir = "outputs",
    ),
)

# O'qitishni boshlash
print("O'qitish boshlanmoqda...")
trainer_stats = trainer.train()

# 5. Modelni saqlash (Merged 4bit yoki LoRA weights)
save_path = os.path.join(base_dir, "models", "operator-base-lora")
print(f"Model {save_path} ga saqlanmoqda...")
model.save_pretrained(save_path)
tokenizer.save_pretrained(save_path)

print("O'qitish muvaffaqiyatli yakunlandi!")
