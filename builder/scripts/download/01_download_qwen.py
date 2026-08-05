import os
from huggingface_hub import hf_hub_download

def download_model():
    repo_id = "Qwen/Qwen2.5-7B-Instruct-GGUF"
    filenames = [
        "qwen2.5-7b-instruct-q4_k_m-00001-of-00002.gguf",
        "qwen2.5-7b-instruct-q4_k_m-00002-of-00002.gguf"
    ]
    
    # Yuklanadigan papkani belgilash
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    models_dir = os.path.join(base_dir, "models")
    os.makedirs(models_dir, exist_ok=True)
    
    for filename in filenames:
        print(f"Yuklanmoqda: {repo_id}/{filename} -> {models_dir}")
        
        local_path = hf_hub_download(
            repo_id=repo_id,
            filename=filename,
            local_dir=models_dir,
            local_dir_use_symlinks=False
        )
        print(f"Yuklash yakunlandi! Fayl manzili: {local_path}")

if __name__ == "__main__":
    download_model()
