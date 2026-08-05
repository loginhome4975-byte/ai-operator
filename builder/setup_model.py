import os
import shutil
import subprocess
import sys

def main():
    print("===================================================")
    print(" AI Operator - LLM Birlashtirish va GGUF Konvertatsiyasi")
    print("===================================================")

    base_dir = "/home/ubuntu/ai-operator"
    builder_dir = os.path.join(base_dir, "builder")
    models_dir = os.path.join(builder_dir, "models")
    lora_out = os.path.join(models_dir, "lora_output")
    merged_out = os.path.join(models_dir, "merged_model")
    final_out = os.path.join(base_dir, "infra_cpu_llm", "model")

    print("1. Papkalar tayyorlanmoqda...")
    os.makedirs(lora_out, exist_ok=True)
    os.makedirs(merged_out, exist_ok=True)
    os.makedirs(final_out, exist_ok=True)

    print("2. Fayllar ko'chirilmoqda...")
    # Simulate moving from downloads if needed
    downloads_dir = "/home/ubuntu/Downloads"
    if os.path.exists(downloads_dir):
        for f in os.listdir(downloads_dir):
            if f.startswith("adapter_") or f.startswith("tokenizer") or f == "chat_template.jinja":
                try:
                    shutil.move(os.path.join(downloads_dir, f), os.path.join(lora_out, f))
                except Exception as e:
                    pass

    adapter_path = os.path.join(lora_out, "adapter_model.safetensors")
    if not os.path.exists(adapter_path):
        print(f"XATOLIK: {adapter_path} topilmadi!")
        sys.exit(1)

    print("3. Modelni birlashtirish (Merging) boshlandi. Kuting...")
    merge_script = os.path.join(builder_dir, "merge_lora.py")
    if os.path.exists(merge_script):
        subprocess.run([sys.executable, merge_script], check=True)
    else:
        print(f"Script topilmadi: {merge_script}")

    print("4. llama.cpp yuklanib GGUF ga o'tkazilmoqda...")
    llama_dir = os.path.join(builder_dir, "llama.cpp")
    if not os.path.exists(llama_dir):
        subprocess.run(["git", "clone", "https://github.com/ggerganov/llama.cpp", llama_dir], check=True)
        subprocess.run([sys.executable, "-m", "pip", "install", "-r", os.path.join(llama_dir, "requirements.txt")], check=True)
    
    print("5. GGUF (8-bit) Konvertatsiya qilinmoqda...")
    convert_script = os.path.join(llama_dir, "convert_hf_to_gguf.py")
    out_gguf = os.path.join(final_out, "miyya-qwen-7b-q8_0.gguf")
    subprocess.run([
        sys.executable, convert_script, merged_out, 
        "--outfile", out_gguf, 
        "--outtype", "q8_0"
    ], check=True)

    print("===================================================")
    print(" BARCHA JARAYON MUVAFFAQIYATLI YAKUNLANDI! 🎉")
    print(f" GGUF model manzili: {out_gguf}")
    print("===================================================")

if __name__ == "__main__":
    main()
